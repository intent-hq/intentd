//! `ws.file.*` bindings (WSAPI-5).
//!
//! Thin passthroughs to the `WorkspaceApi` file surface: the daemon-side
//! implementations already enforce the workspace-root escape guard the
//! reference `buildFileApi` (`ws-misc-api.ts`) applies, so these bindings
//! only surface the parameters and forward the call.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_str, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.file = {
        read: (path) => host({ method: 'file.read', args: { path } }),
        write: (path, content) => host({ method: 'file.write', args: { path, content } }),
        list: (path) => host({ method: 'file.list', args: { path } }),
        delete: (path) => host({ method: 'file.delete', args: { path } }),
        mkdir: (path) => host({ method: 'file.mkdir', args: { path } }),
        rename: (oldPath, newPath) =>
            host({ method: 'file.rename', args: { oldPath, newPath } }),
        getAttachment: (attachmentId, destDir) =>
            host({ method: 'file.getAttachment', args: { attachmentId, destDir } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&intent_core::AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    let caller = caller_agent_id.cloned();
    match method {
        "read" => {
            let path = req_str(args, "path").map_err(|_| "path is required".to_string())?;
            api.file_read(ws.clone(), path, caller)
                .await
                .map_err(map_err)
        }
        "write" => {
            let path =
                req_str(args, "path").map_err(|_| "path and content are required".to_string())?;
            let content = args
                .get("content")
                .and_then(Value::as_str)
                .ok_or_else(|| "path and content are required".to_string())?
                .to_string();
            api.file_write(ws.clone(), path, content, caller)
                .await
                .map_err(map_err)
        }
        "list" => {
            // `path` defaults to `"."` (reference builder default).
            let path = opt_str(args, "path").unwrap_or_else(|| ".".to_string());
            api.file_list(ws.clone(), path, caller)
                .await
                .map_err(map_err)
        }
        "delete" => {
            let path = req_str(args, "path").map_err(|_| "path is required".to_string())?;
            api.file_delete(ws.clone(), path, caller)
                .await
                .map_err(map_err)
        }
        "mkdir" => {
            let path = req_str(args, "path").map_err(|_| "path is required".to_string())?;
            api.file_mkdir(ws.clone(), path, caller)
                .await
                .map_err(map_err)
        }
        "rename" => {
            let old_path = req_str(args, "oldPath")
                .map_err(|_| "Both oldPath and newPath are required".to_string())?;
            let new_path = req_str(args, "newPath")
                .map_err(|_| "Both oldPath and newPath are required".to_string())?;
            api.file_rename(ws.clone(), old_path, new_path, caller)
                .await
                .map_err(map_err)
        }
        "getAttachment" => {
            // Copy a registered attachment from the canonical store into the
            // CALLER's working directory (sandbox clone for CoW-sandboxed
            // callers — resolved inside the service from the caller session).
            let attachment_id = req_str(args, "attachmentId")
                .map_err(|_| "attachmentId is required".to_string())?;
            let dest_dir = opt_str(args, "destDir");
            api.file_get_attachment(ws.clone(), attachment_id, caller, dest_dir)
                .await
                .map_err(map_err)
        }
        other => Err(format!("host: unknown method `file.{other}`")),
    }
}
