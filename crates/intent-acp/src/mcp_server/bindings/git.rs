//! `ws.git.*` bindings (WSAPI-5).
//!
//! The namespace exposes a single binding: `git.commit`, the agent-attributed
//! commit helper (formerly `git.agentCommit`). It requires caller-agent
//! context so attribution never falls back to an anonymous commit, auto-stages
//! only the caller's own changes, and honors the workspace auto-commit policy
//! (`userRequested: true` bypasses a disabled toggle). Read/stage/diff-style
//! git operations are intentionally unbound — agents use the plain `git` CLI
//! instead.

use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use super::{map_err, opt_bool, opt_vec_str, req_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.git = {
        commit: (message, opts) =>
            host({ method: 'git.commit', args: { message, ...(opts || {}) } }),
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
