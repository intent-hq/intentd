//! `ws.git.*` bindings (WSAPI-5).
//!
//! The namespace exposes `git.commit`, the agent-attributed commit helper
//! (formerly `git.agentCommit`). It requires caller-agent context so
//! attribution never falls back to an anonymous commit, auto-stages only the
//! caller's own changes, and honors the workspace auto-commit policy
//! (`userRequested: true` bypasses a disabled toggle). Read/stage/diff-style
//! git operations are intentionally unbound — agents use the plain `git` CLI
//! instead.
//!
//! It also exposes the multi git root registration surface (monorepo#2053):
//! `git.registerRoot` / `git.unregisterRoot` / `git.listRoots` let agents
//! register secondary git repositories (submodule checkouts, sibling clones)
//! for the workspace's git root tracking. `registerRoot` requires caller
//! context for attribution; the other two do not.

use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use super::{map_err, opt_bool, opt_vec_str, req_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.git = {
        commit: (message, opts) =>
            host({ method: 'git.commit', args: { message, ...(opts || {}) } }),
        registerRoot: (path) =>
            host({ method: 'git.registerRoot', args: { path } }),
        unregisterRoot: (path) =>
            host({ method: 'git.unregisterRoot', args: { path } }),
        listRoots: () =>
            host({ method: 'git.listRoots', args: {} }),
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
        "commit" => commit(api, ws, caller_agent_id, args).await,
        "registerRoot" => register_root(api, ws, caller_agent_id, args).await,
        "unregisterRoot" => unregister_root(api, ws, args).await,
        "listRoots" => list_roots(api, ws).await,
        other => Err(format!("host: unknown method `git.{other}`")),
    }
}

async fn commit(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let message = req_str(args, "message").map_err(|_| "message is required".to_string())?;
    let agent_id = caller_agent_id.cloned().ok_or_else(|| {
        "No agent context available. This tool must be called by an agent.".to_string()
    })?;
    let files = opt_vec_str(args, "files");
    let user_requested = opt_bool(args, "userRequested").unwrap_or(false);
    let r = api
        .git_agent_commit(
            ws.clone(),
            message,
            Some(agent_id),
            None,
            files,
            user_requested,
        )
        .await
        .map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "hash": r.hash,
        "files": r.files,
        "fileCount": r.file_count,
    }))
}

async fn register_root(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let path = req_str(args, "path").map_err(|_| "path is required".to_string())?;
    let agent_id = caller_agent_id.cloned().ok_or_else(|| {
        "No agent context available. This tool must be called by an agent.".to_string()
    })?;
    api.git_root_register(ws.clone(), path, agent_id)
        .await
        .map_err(map_err)
}

async fn unregister_root(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let path = req_str(args, "path").map_err(|_| "path is required".to_string())?;
    api.git_root_unregister(ws.clone(), path)
        .await
        .map_err(map_err)
}

async fn list_roots(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    // Unwrap the wire envelope — `ws.git.listRoots()` returns the array
    // directly, matching the reference docs.
    let v = api.git_root_list(ws.clone()).await.map_err(map_err)?;
    Ok(v.get("gitRoots").cloned().unwrap_or_else(|| json!([])))
}
