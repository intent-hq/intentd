//! `ws.crossWorkspace.*` bindings (WSAPI-6).
//!
//! Thin wrappers over the [`WorkspaceApi`] cross-workspace surface: sibling
//! discovery and cross-workspace note reads. The daemon already enforces the
//! "same repositoryPath" access rule and shapes the returned payload — the
//! binding only peels arguments and echoes the trait's `serde_json::Value`
//! result unchanged (reference parity with `buildCrossWorkspaceApi` in
//! `ws-misc-api.ts`).

use std::sync::Arc;

use intent_core::{NoteId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.crossWorkspace = {
        listSiblings: () => host({ method: 'crossWorkspace.listSiblings', args: {} }),
        readNote: (targetWorkspaceId, noteId) =>
            host({ method: 'crossWorkspace.readNote', args: { targetWorkspaceId, noteId } }),
        listNotes: (targetWorkspaceId) =>
            host({ method: 'crossWorkspace.listNotes', args: { targetWorkspaceId } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "listSiblings" => list_siblings(api, ws).await,
        "readNote" => read_note(api, ws, args).await,
        "listNotes" => list_notes(api, ws, args).await,
        other => Err(format!("host: unknown method `crossWorkspace.{other}`")),
    }
}

async fn list_siblings(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    api.cross_workspace_list_siblings(ws.clone())
        .await
        .map_err(map_err)
}

async fn read_note(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let target = req_str(args, "targetWorkspaceId")
        .map_err(|_| "Both workspaceId and noteId are required".to_string())?;
    let note_id = req_str(args, "noteId")
        .map_err(|_| "Both workspaceId and noteId are required".to_string())?;
    api.cross_workspace_read_note(
        ws.clone(),
        WorkspaceId::from_string(target),
        NoteId::from_string(&note_id),
    )
    .await
    .map_err(map_err)
}

async fn list_notes(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let target =
        req_str(args, "targetWorkspaceId").map_err(|_| "workspaceId is required".to_string())?;
    api.cross_workspace_list_notes(ws.clone(), WorkspaceId::from_string(target))
        .await
        .map_err(map_err)
}
