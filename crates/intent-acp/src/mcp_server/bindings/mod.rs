//! Per-namespace `workspace_api` bindings (WSAPI-3+).
//!
//! Each submodule owns:
//!  * a `PRELUDE` JS fragment that populates its `ws.<ns>` object, and
//!  * a `dispatch` fn that routes one `host({ method, args })` frame's
//!    method-suffix (the part after `"<ns>."`) to the shared
//!    [`WorkspaceApi`].
//!
//! `dispatch.rs` concatenates every namespace prelude into the single JS
//! prelude installed before user code, and delegates every host frame here.
//! Splitting the surface this way lets each future WSAPI wave own one file
//! without touching the shared bootstrap.

use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

pub(crate) mod agent;
pub(crate) mod app;
pub(crate) mod browser;
pub(crate) mod comment;
pub(crate) mod cross_workspace;
pub(crate) mod event;
pub(crate) mod file;
pub(crate) mod git;
pub(crate) mod note;
pub(crate) mod pr;
pub(crate) mod primitive;
pub(crate) mod script;
pub(crate) mod task;
pub(crate) mod terminal;
pub(crate) mod workspace;

/// Build the JS installed before user code. Each per-namespace fragment
/// attaches its `ws.<ns>` object next to the others on the shared `ws`
/// global. Concatenation happens at call time because the per-namespace
/// fragments are `const &str` expressions, and `concat!` only accepts
/// literals.
pub(crate) fn prelude() -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
        workspace::PRELUDE,
        note::PRELUDE,
        task::PRELUDE,
        comment::PRELUDE,
        primitive::PRELUDE,
        cross_workspace::PRELUDE,
        pr::PRELUDE,
        browser::PRELUDE,
        agent::PRELUDE,
        event::PRELUDE,
        git::PRELUDE,
        script::PRELUDE,
        terminal::PRELUDE,
        file::PRELUDE,
        app::prelude(),
    )
}

/// Dispatch one `host({ method, args })` frame to the matching per-namespace
/// handler. Returns `Ok(None)` when the namespace is not owned here (unknown
/// method); `Ok(Some(v))` on success and `Err(msg)` on a JS-visible failure.
/// `caller_agent_id` threads the tool-call's agent context so the bindings
/// that attribute their calls back to the spawning agent
/// (`workspace.setAgentName` / `git.commit` / `git.agentCommit` /
/// `ws.browser.exec`, and the caller-aware `ws.agent.*` methods — `create`,
/// `delegate`, `send`, `sendToTask`, `wakeOrCreate`, `reportToParent`) can do so.
pub(crate) async fn try_dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller_agent_id: &Option<AgentId>,
    method: &str,
    args: &Value,
) -> Result<Option<Value>, String> {
    if let Some(rest) = method.strip_prefix("workspace.") {
        return workspace::dispatch(api, workspace_id, caller_agent_id.as_ref(), rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("note.") {
        return note::dispatch(api, workspace_id, caller_agent_id.as_ref(), rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("task.") {
        return task::dispatch(api, workspace_id, caller_agent_id.as_ref(), rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("comment.") {
        return comment::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("primitive.") {
        return primitive::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("crossWorkspace.") {
        return cross_workspace::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("pr.") {
        return pr::dispatch(api, workspace_id, rest, args).await.map(Some);
    }
    if let Some(rest) = method.strip_prefix("browser.") {
        return browser::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("agent.") {
        return agent::dispatch(api, workspace_id, caller_agent_id.as_ref(), rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("event.") {
        return event::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("git.") {
        return git::dispatch(api, workspace_id, caller_agent_id.as_ref(), rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("script.") {
        return script::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("terminal.") {
        return terminal::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("file.") {
        return file::dispatch(api, workspace_id, caller_agent_id.as_ref(), rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("app.") {
        return app::try_dispatch(api, workspace_id, rest, args).await;
    }
    Ok(None)
}

/// Pull a required string field from a JS `args` object, surfacing the same
/// "X is required" style errors as the TS reference bindings.
pub(crate) fn req_str(args: &Value, key: &str) -> Result<String, String> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("{key} is required"))
}

/// Pull an optional string field from a JS `args` object.
pub(crate) fn opt_str(args: &Value, key: &str) -> Option<String> {
    args.get(key).and_then(Value::as_str).map(str::to_string)
}

/// Pull a required i64 field, tolerating JS numbers that arrived as strings
/// (some reference builders accept `"12"` alongside `12`).
pub(crate) fn req_i64(args: &Value, key: &str) -> Result<i64, String> {
    if let Some(v) = args.get(key).and_then(Value::as_i64) {
        return Ok(v);
    }
    if let Some(s) = args.get(key).and_then(Value::as_str) {
        return s
            .parse::<i64>()
            .map_err(|_| format!("{key} must be an integer"));
    }
    Err(format!("{key} is required"))
}

/// Pull an optional bool field.
pub(crate) fn opt_bool(args: &Value, key: &str) -> Option<bool> {
    args.get(key).and_then(Value::as_bool)
}

/// Pull an optional i64 field, tolerating JS numbers that arrived as strings.
pub(crate) fn opt_i64(args: &Value, key: &str) -> Option<i64> {
    if let Some(v) = args.get(key).and_then(Value::as_i64) {
        return Some(v);
    }
    args.get(key)
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<i64>().ok())
}

/// Pull an optional string-array field, either as a JSON array of strings or
/// as a comma-separated string (the TS `normalizeTags` fallback).
pub(crate) fn opt_vec_str(args: &Value, key: &str) -> Option<Vec<String>> {
    if let Some(a) = args.get(key).and_then(Value::as_array) {
        return Some(
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
        );
    }
    if let Some(s) = args.get(key).and_then(Value::as_str) {
        return Some(
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
        );
    }
    None
}

/// Convert a [`intent_core::Error`] into the JS-visible error text used
/// throughout these bindings (the trait's `Display` impl already renders the
/// message content the reference builders threw).
pub(crate) fn map_err(e: intent_core::Error) -> String {
    e.to_string()
}
