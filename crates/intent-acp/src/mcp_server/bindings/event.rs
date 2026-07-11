//! `ws.event.*` bindings (WSAPI-4).
//!
//! Thin JS wrappers around `host({ method, args })` that route to the shared
//! [`WorkspaceApi`] event surface (§5.10). The subscribe wildcard `"*"` is
//! expanded here — parity with the reference `ws-event-api.ts` — into the
//! documented category list before it reaches the daemon.

use std::sync::Arc;

use intent_core::{EventQueryParams, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_bool, opt_str, opt_vec_str, req_str};

/// Category wildcards the reference `ws.event.subscribe` expands `"*"` into.
/// Kept in-sync with `VALID_EVENT_CATEGORY_WILDCARDS` in the TS builder.
const VALID_EVENT_CATEGORY_WILDCARDS: &[&str] = &[
    "agent:*",
    "file:*",
    "task:*",
    "git:*",
    "note:*",
    "terminal:*",
    "test:*",
    "build:*",
    "workspace:*",
    "spec:*",
    "goal:*",
    "comment:*",
];

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.event = {
        recentFiles: (limit) => host({ method: 'event.recentFiles', args: { limit } }),
        agentActivity: (agentId, minutesAgo) =>
            host({ method: 'event.agentActivity', args: { agentId, minutesAgo } }),
        workspaceSummary: (minutesAgo) =>
            host({ method: 'event.workspaceSummary', args: { minutesAgo } }),
        directoryChanges: (dir, limit) =>
            host({ method: 'event.directoryChanges', args: { dir, limit } }),
        query: (options) => host({ method: 'event.query', args: { ...(options || {}) } }),
        subscribe: (eventTypes, opts) =>
            host({ method: 'event.subscribe', args: { eventTypes, ...(opts || {}) } }),
        unsubscribe: (subscriptionId) =>
            host({ method: 'event.unsubscribe', args: { subscriptionId } }),
    };
"#;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "recentFiles" => recent_files(api, ws, args).await,
        "agentActivity" => agent_activity(api, ws, args).await,
        "workspaceSummary" => workspace_summary(api, ws, args).await,
        "directoryChanges" => directory_changes(api, ws, args).await,
        "query" => query(api, ws, args).await,
        "subscribe" => subscribe(api, ws, args).await,
        "unsubscribe" => unsubscribe(api, ws, args).await,
        other => Err(format!("host: unknown method `event.{other}`")),
    }
}

async fn recent_files(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let limit = args.get("limit").and_then(Value::as_i64);
    let rows = api
        .event_recent_files(ws.clone(), limit)
        .await
        .map_err(map_err)?;
    serde_json::to_value(rows).map_err(|e| e.to_string())
}

async fn agent_activity(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id = opt_str(args, "agentId");
    let minutes_ago = args.get("minutesAgo").and_then(Value::as_i64);
    api.event_agent_activity(ws.clone(), agent_id, minutes_ago)
        .await
        .map_err(map_err)
}

async fn workspace_summary(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let minutes_ago = args.get("minutesAgo").and_then(Value::as_i64);
    let summary = api
        .event_workspace_summary(ws.clone(), minutes_ago)
        .await
        .map_err(map_err)?;
    serde_json::to_value(summary).map_err(|e| e.to_string())
}

async fn directory_changes(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let dir = req_str(args, "dir").map_err(|_| "Directory path is required".to_string())?;
    let limit = args.get("limit").and_then(Value::as_i64);
    let rows = api
        .event_directory_changes(ws.clone(), dir, limit)
        .await
        .map_err(map_err)?;
    serde_json::to_value(rows).map_err(|e| e.to_string())
}

async fn query(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let params = EventQueryParams {
        event_type: opt_str(args, "eventType"),
        actor_type: opt_str(args, "actorType"),
        actor_id: opt_str(args, "actorId"),
        path: opt_str(args, "path"),
        minutes_ago: args.get("minutesAgo").and_then(Value::as_i64),
        limit: args.get("limit").and_then(Value::as_i64),
        paginate: opt_bool(args, "paginate"),
        page_token: opt_str(args, "pageToken"),
    };
    api.event_query(ws.clone(), params).await.map_err(map_err)
}

async fn subscribe(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let event_types = opt_vec_str(args, "eventTypes").ok_or_else(|| {
        "eventTypes is required. Specify category wildcards like \"agent:*\", \"file:*\" or specific types like \"agent:idle\".".to_string()
    })?;
    if event_types.is_empty() {
        return Err(
            "eventTypes is required. Specify category wildcards like \"agent:*\", \"file:*\" or specific types like \"agent:idle\".".to_string(),
        );
    }
    let resolved = expand_wildcards(&event_types);
    let v = api
        .agent_subscribe(
            ws.clone(),
            resolved,
            opt_bool(args, "excludeSelf"),
            args.get("batchWindow").and_then(Value::as_i64),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(v).map_err(|e| e.to_string())
}

async fn unsubscribe(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let subscription_id =
        req_str(args, "subscriptionId").map_err(|_| "subscriptionId is required".to_string())?;
    let r = api
        .event_unsubscribe(ws.clone(), subscription_id)
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

fn expand_wildcards(event_types: &[String]) -> Vec<String> {
    let mut resolved = Vec::with_capacity(event_types.len());
    for t in event_types {
        if t == "*" {
            resolved.extend(VALID_EVENT_CATEGORY_WILDCARDS.iter().map(|s| s.to_string()));
        } else {
            resolved.push(t.clone());
        }
    }
    resolved
}
