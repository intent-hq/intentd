//! `ws.workspace.*` bindings (WSAPI-5).
//!
//! Extends the WSAPI-2 `workspace.info` proof point with the reference
//! `ws-workspace-api.ts` surface. Methods without a backing daemon RPC
//! (`context`, `referenceDocs`, `emitNotification`, and the timeline data
//! source itself) surface a clear "not yet available in port" error so the
//! JS caller sees why the binding cannot resolve, instead of inventing
//! behavior.

use std::sync::Arc;

use intent_core::{
    AgentId, Error, WorkspaceApi, WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
    WORKSPACE_STATUS_MESSAGE_MAX_LENGTH,
};
use serde_json::{json, Value};

use super::{map_err, req_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.workspace = {
        info: () => host({ method: 'workspace.info' }),
        details: () => host({ method: 'workspace.details' }),
        setTitle: (title) => host({ method: 'workspace.setTitle', args: { title } }),
        setStatusMessage: (statusMessage) =>
            host({ method: 'workspace.setStatusMessage', args: { statusMessage } }),
        setAgentName: (name) => host({ method: 'workspace.setAgentName', args: { name } }),
        context: () => host({ method: 'workspace.context' }),
        timeline: (limit, type) =>
            host({ method: 'workspace.timeline', args: { limit, type } }),
        referenceDocs: (topic) =>
            host({ method: 'workspace.referenceDocs', args: { topic } }),
        emitNotification: (topic, message, metadata) =>
            host({ method: 'workspace.emitNotification', args: { topic, message, metadata } }),
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
        "info" => info(api, ws).await,
        "details" => details(api, ws).await,
        "setTitle" => set_title(api, ws, args).await,
        "setStatusMessage" => set_status_message(api, ws, args).await,
        "setAgentName" => set_agent_name(api, caller_agent_id, args).await,
        "context" => {
            Err("ws.workspace.context is not yet available in this daemon port".to_string())
        }
        "timeline" => {
            Err("ws.workspace.timeline is not yet available in this daemon port".to_string())
        }
        "referenceDocs" => {
            Err("ws.workspace.referenceDocs is not yet available in this daemon port".to_string())
        }
        "emitNotification" => Err(
            "ws.workspace.emitNotification is not yet available in this daemon port".to_string(),
        ),
        other => Err(format!("host: unknown method `workspace.{other}`")),
    }
}

async fn info(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    let workspace = api.get_workspace(ws.clone()).await.map_err(map_err)?;
    let path = workspace.path.clone().or(workspace.worktree_path.clone());
    Ok(json!({
        "id": ws.as_str(),
        "path": path,
    }))
}

async fn details(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    match api.get_workspace(ws.clone()).await {
        Ok(w) => {
            let title = w.title.trim();
            let has_title = !title.is_empty() && title != w.id.as_str();
            Ok(json!({
                "id": w.id.as_str(),
                "title": if title.is_empty() { "(untitled)" } else { title },
                "hasTitle": has_title,
                "status": w.status,
                "statusMessage": w.status_message,
                "branch": w.branch,
                "repositoryName": w.repository_name,
                "tags": w.tags,
            }))
        }
        Err(Error::NotFound(_)) => Ok(json!({
            "id": ws.as_str(),
            "title": "(untitled)",
            "hasTitle": false,
            "status": WorkspaceStatus::Active,
            "statusMessage": Value::Null,
            "branch": Value::Null,
            "repositoryName": Value::Null,
            "tags": Vec::<String>::new(),
        })),
        Err(e) => Err(e.to_string()),
    }
}

async fn set_title(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let title = req_str(args, "title").map_err(|_| "title is required".to_string())?;
    let trimmed = title.trim().to_string();
    if trimmed.is_empty() {
        return Err("title is required".to_string());
    }
    let existing = api.get_workspace(ws.clone()).await.map_err(map_err)?;
    let existing_title = existing.title.trim();
    if !existing_title.is_empty() && existing_title != existing.id.as_str() {
        return Ok(json!({
            "ok": true,
            "skipped": true,
            "title": existing_title,
            "branch": existing.branch,
        }));
    }
    let update = WorkspaceUpdate {
        title: Some(trimmed.clone()),
        ..Default::default()
    };
    let updated = api
        .update_workspace(ws.clone(), update)
        .await
        .map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "title": updated.title,
        "branch": updated.branch,
    }))
}

async fn set_status_message(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let raw = match args.get("statusMessage") {
        Some(Value::Null) | None => String::new(),
        Some(Value::String(s)) => s.clone(),
        _ => return Err("statusMessage must be a string or null".to_string()),
    };
    let trimmed = raw.trim().to_string();
    // The reference contract (`WORKSPACE_STATUS_MESSAGE_MAX_LENGTH`,
    // `src/shared/types.ts`) is a *character* limit, not a byte limit —
    // count Unicode scalars via `chars()` so multi-byte characters (emoji,
    // CJK, etc.) are not rejected well below 500 characters.
    if trimmed.chars().count() > WORKSPACE_STATUS_MESSAGE_MAX_LENGTH {
        return Err(format!(
            "statusMessage must be {WORKSPACE_STATUS_MESSAGE_MAX_LENGTH} characters or fewer"
        ));
    }
    let update = WorkspaceUpdate {
        status_message: Some(trimmed.clone()),
        ..Default::default()
    };
    let updated = api
        .update_workspace(ws.clone(), update)
        .await
        .map_err(map_err)?;
    // Preserve the `Option<String>` shape end-to-end: `None` maps to
    // `Value::Null`, `Some(v)` to `Value::String(v)`. Never collapse to `""`
    // via `unwrap_or_default()` — that would conflate a cleared value with
    // an explicitly empty string and reintroduce the exact empty-vs-null
    // mismatch the services-side clear normalization is fixing.
    let out = updated
        .status_message
        .map(Value::String)
        .unwrap_or(Value::Null);
    Ok(json!({ "ok": true, "statusMessage": out }))
}

async fn set_agent_name(
    api: &Arc<dyn WorkspaceApi>,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let name = req_str(args, "name").map_err(|_| "name is required".to_string())?;
    let agent_id = caller_agent_id
        .cloned()
        .ok_or_else(|| "Could not determine agent ID from request context".to_string())?;
    let r = api
        .agent_rename(agent_id, name, true)
        .await
        .map_err(map_err)?;
    Ok(r)
}
