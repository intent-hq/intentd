//! `ws.git.*` bindings (WSAPI-5).
//!
//! Thin wrappers over the `WorkspaceApi` git surface, mirroring the reference
//! `ws-git-api.ts` builder. `git.stage` refuses the reference stage-all
//! sentinels (`"."`, `"*"`, or a string containing `--all`) with the reference
//! error text before delegating to the daemon; `git.agentCommit` requires
//! caller-agent context so attribution never falls back to an anonymous
//! commit.

use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use super::{map_err, opt_bool, opt_str, opt_vec_str, req_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.git = {
        status: () => host({ method: 'git.status' }),
        stage: (paths) => host({ method: 'git.stage', args: { paths } }),
        commit: (message) => host({ method: 'git.commit', args: { message } }),
        agentCommit: (message, opts) =>
            host({ method: 'git.agentCommit', args: { message, ...(opts || {}) } }),
        checkMergeConflicts: (targetBranch) =>
            host({ method: 'git.checkMergeConflicts', args: { targetBranch } }),
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
        "status" => status(api, ws).await,
        "stage" => stage(api, ws, args).await,
        "commit" => commit(api, ws, caller_agent_id, args).await,
        "agentCommit" => agent_commit(api, ws, caller_agent_id, args).await,
        "checkMergeConflicts" => check_merge_conflicts(api, ws, args).await,
        other => Err(format!("host: unknown method `git.{other}`")),
    }
}

async fn status(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    let r = api.git_status(ws.clone()).await.map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn stage(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let paths = args
        .get("paths")
        .cloned()
        .ok_or_else(|| "paths is required".to_string())?;
    // Stage-all sentinels are rejected here — mirrors `ws-git-api.ts` verbatim
    // so agents cannot bypass the guard via the tool-restrictions layer.
    if let Some(s) = paths.as_str() {
        if s == "." || s == "*" || s.contains("--all") {
            return Err(
                "Staging all files is not allowed. Please specify individual file paths to stage. \
                 Use git_status to see which files you have modified, then stage only those \
                 specific files."
                    .to_string(),
            );
        }
    }
    let list: Vec<String> = if let Some(arr) = paths.as_array() {
        arr.iter()
            .filter_map(Value::as_str)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else if let Some(s) = paths.as_str() {
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    } else {
        return Err("paths must be a string or array of strings".to_string());
    };
    if list.is_empty() {
        return Err(
            "No file paths provided. Please specify at least one file path to stage.".to_string(),
        );
    }
    let staged = api
        .git_stage(
            ws.clone(),
            Value::Array(list.iter().cloned().map(Value::from).collect()),
        )
        .await
        .map_err(map_err)?;
    Ok(json!({ "ok": true, "paths": staged }))
}

async fn commit(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let message = req_str(args, "message").map_err(|_| "message is required".to_string())?;
    // Match the reference: append the caller's `Agent-Id` trailer when present.
    let full_message = if let Some(agent) = caller_agent_id {
        format!("{message}\n\nAgent-Id: {}", agent.as_str())
    } else {
        message
    };
    let r = api
        .git_commit(ws.clone(), full_message, None)
        .await
        .map_err(map_err)?;
    Ok(json!({ "ok": true, "hash": r.hash, "files": r.files }))
}

async fn agent_commit(
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

async fn check_merge_conflicts(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let target = opt_str(args, "targetBranch");
    let r = api
        .git_check_merge_conflicts(ws.clone(), target)
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}
