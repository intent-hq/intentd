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
    AgentId, AgentStatus, Error, WorkspaceApi, WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
    WORKSPACE_STATUS_MESSAGE_MAX_LENGTH,
};
use serde_json::{json, Value};

use super::{map_err, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.workspace = {
        info: () => host({ method: 'workspace.info' }),
        details: () => host({ method: 'workspace.details' }),
        setTitle: (title) => host({ method: 'workspace.setTitle', args: { title } }),
        setStatusMessage: (statusMessage) =>
            host({ method: 'workspace.setStatusMessage', args: { statusMessage } }),
        setStatusImage: (image) =>
            host({ method: 'workspace.setStatusImage', args: { image } }),
        setAgentName: (name) => host({ method: 'workspace.setAgentName', args: { name } }),
        archive: () => host({ method: 'workspace.archive' }),
        unarchive: () => host({ method: 'workspace.unarchive' }),
        context: () => host({ method: 'workspace.context' }),
        timeline: (limit, type) =>
            host({ method: 'workspace.timeline', args: { limit, type } }),
        referenceDocs: (topic) =>
            host({ method: 'workspace.referenceDocs', args: { topic } }),
        emitNotification: (topic, message, metadata) =>
            host({ method: 'workspace.emitNotification', args: { topic, message, metadata } }),
    };
";

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
        "setStatusImage" => set_status_image(api, ws, args).await,
        "setAgentName" => set_agent_name(api, caller_agent_id, args).await,
        "archive" => archive(api, ws, caller_agent_id).await,
        "unarchive" => unarchive(api, ws).await,
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
            // Legacy rows persisted before the services-layer clear
            // normalization (and any other writer that still emits `""`
            // or whitespace) can leak an empty string here, which would
            // break the documented clear contract (`empty/null ⇒ null`).
            // Normalize on read so `details()` always surfaces `null`
            // for a cleared status message.
            let status_message = w
                .status_message
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null);
            Ok(json!({
                "id": w.id.as_str(),
                "title": if title.is_empty() { "(untitled)" } else { title },
                "hasTitle": has_title,
                "status": w.status,
                "statusMessage": status_message,
                "statusImageAssetId": w.status_image_asset_id,
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
            "statusImageAssetId": Value::Null,
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

/// `ws.workspace.setStatusImage({ data, mimeType, originalName? } | null)`
/// (intent-hq/monorepo#997 part 1): store an agent-authored status screenshot
/// through the content-addressed asset machinery (`note.saveAsset`) and point
/// `Workspace.statusImageAssetId` at it; `null` clears the reference. The
/// asset write happens BEFORE the workspace update so a failed save never
/// leaves a dangling asset id on the row.
async fn set_status_image(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    chief_guard(ws, "setStatusImage")?;
    // Missing vs explicit `null` matters: a clear is destructive, so a no-arg
    // call (the prelude's JSON.stringify drops `undefined` keys) errors
    // instead of silently clearing — only an explicit `null` clears.
    let Some(image) = args.get("image") else {
        return Err(
            "image is required: pass { data, mimeType, originalName? } to set or null to clear"
                .to_string(),
        );
    };
    if image.is_null() {
        let update = WorkspaceUpdate {
            status_image_asset_id: Some(None),
            ..Default::default()
        };
        api.update_workspace(ws.clone(), update)
            .await
            .map_err(map_err)?;
        return Ok(json!({ "ok": true, "statusImageAssetId": Value::Null }));
    }
    let Some(obj) = image.as_object() else {
        return Err(
            "image must be an object { data, mimeType, originalName? } or null".to_string(),
        );
    };
    let data = obj
        .get("data")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "image.data (base64) is required".to_string())?;
    let mime_type = obj
        .get("mimeType")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "image.mimeType is required".to_string())?;
    if !mime_type.starts_with("image/") {
        return Err(format!(
            "image.mimeType must be an image/* type, got `{mime_type}`"
        ));
    }
    let original_name = obj
        .get("originalName")
        .and_then(Value::as_str)
        .map(str::to_string);
    let saved = api
        .save_asset(
            ws.clone(),
            data.to_string(),
            mime_type.to_string(),
            original_name,
        )
        .await
        .map_err(map_err)?;
    let update = WorkspaceUpdate {
        status_image_asset_id: Some(Some(saved.asset_id.clone())),
        ..Default::default()
    };
    api.update_workspace(ws.clone(), update)
        .await
        .map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "statusImageAssetId": saved.asset_id,
        "url": saved.url,
    }))
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

/// The chief workspace is virtual and cannot be archived/unarchived. The
/// service methods silently no-op for chief (they return the synthesized
/// `chief_workspace()` unchanged), which would misleadingly look like
/// success to the agent — so the refusal lives here in the binding layer.
fn chief_guard(ws: &WorkspaceId, method: &str) -> Result<(), String> {
    if ws.is_chief() {
        return Err(format!(
            "ws.workspace.{method} is not available in the chief-of-staff workspace"
        ));
    }
    Ok(())
}

/// Whether an [`intent_core::AgentLite`] projection counts as running/queued
/// for the archive guardrail: the daemon-owned in-flight signal
/// (`is_responding`) or a persisted in-flight/queued status. `RuntimeIdle`,
/// `Idle`, `Completed`, `Error`, and `Deleted` sessions do not block.
fn is_running_or_queued(agent: &intent_core::AgentLite) -> bool {
    agent.is_responding
        || matches!(
            agent.status,
            AgentStatus::Pending
                | AgentStatus::Active
                | AgentStatus::Processing
                | AgentStatus::Waiting
        )
}

async fn archive(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
) -> Result<Value, String> {
    chief_guard(ws, "archive")?;
    // Hard refusal when OTHER agents are running/queued — no force override.
    // The calling agent is necessarily mid-turn, so it is excluded.
    let agents = api.agent_list(ws.clone()).await.map_err(map_err)?;
    let blocking: Vec<String> = agents
        .iter()
        .filter(|a| Some(&a.id) != caller_agent_id)
        .filter(|a| is_running_or_queued(a))
        .map(|a| format!("{} ({})", a.name, a.id.as_str()))
        .collect();
    if !blocking.is_empty() {
        return Err(format!(
            "Cannot archive: {} other agent(s) running or queued in this workspace: {}. \
             Wait for them to finish or stop them first.",
            blocking.len(),
            blocking.join(", ")
        ));
    }
    // The caller rides along so the service-layer interrupt sweep skips it:
    // the calling agent is mid-turn awaiting this tool result, and
    // interrupting it would abort the worker owning this dispatch.
    let updated = api
        .archive_workspace(ws.clone(), caller_agent_id.cloned())
        .await
        .map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "status": updated.status,
        "archivedAt": updated.archived_at,
    }))
}

async fn unarchive(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    chief_guard(ws, "unarchive")?;
    let updated = api.unarchive_workspace(ws.clone()).await.map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "status": updated.status,
    }))
}
