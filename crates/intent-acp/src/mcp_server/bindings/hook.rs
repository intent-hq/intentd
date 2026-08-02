//! `ws.hook.*` bindings — background hooks (agent-owned scheduled scripts).
//!
//! Thin wrappers over the `WorkspaceApi` hook surface (`hook_manager` in
//! intent-services). `hook.schedule` is MCP-only (there is no wire
//! `hook.schedule` — hooks are agent-authored by design, §6.8) and attributes
//! the calling agent as the hook's owner, so it requires an agent caller
//! context; `list` / `cancel` / `runNow` mirror the wire methods of the same
//! names. An owner-initiated cancel (`by_owner = true`) does not wake the
//! owner — only the FE cancel path does.

use std::sync::Arc;

use intent_core::{AgentId, HookId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, req_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.hook = {
        schedule: (opts) => host({ method: 'hook.schedule', args: opts || {} }),
        list: () => host({ method: 'hook.list' }),
        cancel: (hookId) => host({ method: 'hook.cancel', args: { hookId } }),
        runNow: (hookId) => host({ method: 'hook.runNow', args: { hookId } }),
    };
"#;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "schedule" => schedule(api, ws, caller, args).await,
        "list" => list(api, ws).await,
        "cancel" => cancel(api, ws, args).await,
        "runNow" => run_now(api, ws, args).await,
        other => Err(format!("hook: unknown method `hook.{other}`")),
    }
}

async fn schedule(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    // Hooks are agent-owned: the schedule call must carry an agent caller
    // context (the FE front door and tests dispatch without one).
    let Some(owner) = caller else {
        return Err(
            "hook.schedule requires an agent caller context to attribute ownership".to_string(),
        );
    };
    api.hook_schedule(ws.clone(), owner.clone(), args.clone())
        .await
        .map_err(map_err)
}

async fn list(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    let raw = api.hook_list(ws.clone(), None).await.map_err(map_err)?;
    // The service returns `{ hooks: [...] }` (the wire shape); JS callers get
    // the bare array, mirroring `ws.script.list`.
    if let Some(inner) = raw.get("hooks") {
        return Ok(inner.clone());
    }
    Ok(raw)
}

async fn cancel(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let hook_id = req_str(args, "hookId").map_err(|_| "hookId is required".to_string())?;
    api.hook_cancel(ws.clone(), HookId::from(hook_id.as_str()), true)
        .await
        .map_err(map_err)
}

async fn run_now(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let hook_id = req_str(args, "hookId").map_err(|_| "hookId is required".to_string())?;
    api.hook_run_now(ws.clone(), HookId::from(hook_id.as_str()))
        .await
        .map_err(map_err)
}
