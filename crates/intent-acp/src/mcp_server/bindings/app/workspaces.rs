//! `ws.app.workspaces.*` bindings (chief-gated).
//!
//! Exposes workspace management methods (`list`, `get`) exclusively to
//! Chief-of-Staff workspace agents. Non-chief agents receive a clear gating
//! error. Shape parity with the TS reference
//! `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-workspaces-api.ts`.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::mcp_server::bindings::{map_err, opt_bool, opt_str, opt_vec_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.workspaces = {
        list: (options) => host({ method: 'app.workspaces.list', args: options || {} }),
        get: (id) => host({ method: 'app.workspaces.get', args: { id } }),
    };
"#;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    // Chief-workspace gating: all ws.app.* methods require the caller to be
    // in the Chief workspace.
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Chief of Staff workspace".to_string());
    }

    match method {
        "list" => list(api, args).await,
        "get" => get(api, args).await,
        other => Err(format!("host: unknown method `app.workspaces.{other}`")),
    }
}

async fn list(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    // Extract filter options
    let filter_obj = args.get("filter");
    let query = filter_obj.and_then(|f| opt_str(f, "query").or_else(|| opt_str(f, "search")));
    let status_filter = filter_obj.and_then(|f| {
        f.get("status").and_then(|s| {
            if let Some(arr) = s.as_array() {
                Some(
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_lowercase()))
                        .collect::<Vec<_>>(),
                )
            } else {
                s.as_str().map(|s| vec![s.to_lowercase()])
            }
        })
    });
    let repository_path = filter_obj.and_then(|f| opt_str(f, "repositoryPath"));
    let repository_owner = filter_obj.and_then(|f| opt_str(f, "repositoryOwner"));
    let repository_name = filter_obj.and_then(|f| opt_str(f, "repositoryName"));
    let tag = filter_obj.and_then(|f| opt_str(f, "tag"));
    let tags = filter_obj.and_then(|f| opt_vec_str(f, "tags"));
    let include_deleted = filter_obj
        .and_then(|f| opt_bool(f, "includeDeleted"))
        .unwrap_or(false);

    // Fetch all workspaces (include archived so we can apply status filter)
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;

    // Filter and summarize
    let mut results = Vec::new();
    for ws in workspaces {
        // Never surface __chief__ itself
        if ws.id.is_chief() {
            continue;
        }

        // Status filtering
        let status_str = format!("{:?}", ws.status).to_lowercase();
        if !include_deleted && status_str == "deleted" && status_filter.is_none() {
            continue;
        }
        if let Some(ref statuses) = status_filter {
            if !statuses.contains(&status_str) {
                continue;
            }
        }

        // Repository filters
        if let Some(ref path) = repository_path {
            if ws.repository_path.as_deref() != Some(path.as_str()) {
                continue;
            }
        }
        if let Some(ref owner) = repository_owner {
            if ws.repository_owner.as_deref() != Some(owner.as_str()) {
                continue;
            }
        }
        if let Some(ref name) = repository_name {
            if ws.repository_name.as_deref() != Some(name.as_str()) {
                continue;
            }
        }

        // Tag filters
        if let Some(ref single_tag) = tag {
            if !ws.tags.contains(single_tag) {
                continue;
            }
        }
        if let Some(ref tag_list) = tags {
            if !tag_list.iter().all(|t| ws.tags.contains(t)) {
                continue;
            }
        }

        // Query filter (searches across multiple fields)
        if let Some(ref q) = query {
            let q_lower = q.to_lowercase();
            let matches = [
                ws.id.as_str(),
                &ws.title,
                ws.status_message.as_deref().unwrap_or(""),
                &ws.branch,
                ws.repository_path.as_deref().unwrap_or(""),
                ws.repository_owner.as_deref().unwrap_or(""),
                ws.repository_name.as_deref().unwrap_or(""),
            ]
            .iter()
            .any(|field| field.to_lowercase().contains(&q_lower));
            if !matches {
                continue;
            }
        }

        results.push(summarize_workspace(&ws));
    }

    // Apply sort
    if let Some(sort_obj) = args.get("sort") {
        let (sort_by, sort_order) = if let Some(s) = sort_obj.as_str() {
            let order = if s.starts_with('-') { "desc" } else { "asc" };
            let by = s.trim_start_matches('-').to_string();
            (by, order.to_string())
        } else {
            let by = opt_str(sort_obj, "by").unwrap_or_else(|| "updatedAt".to_string());
            let order = opt_str(sort_obj, "order").unwrap_or_else(|| "desc".to_string());
            (by, order)
        };

        results.sort_by(|a, b| {
            let left = a.get(&sort_by).and_then(|v| v.as_str()).unwrap_or("");
            let right = b.get(&sort_by).and_then(|v| v.as_str()).unwrap_or("");
            let cmp = left.to_lowercase().cmp(&right.to_lowercase());
            if sort_order == "asc" {
                cmp
            } else {
                cmp.reverse()
            }
        });
    }

    Ok(Value::Array(results))
}

async fn get(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    let workspace = api
        .get_workspace(WorkspaceId::from_string(id.to_string()))
        .await
        .map_err(map_err)?;

    // Never surface __chief__ via ws.app.workspaces.get
    if workspace.id.is_chief() {
        return Err(format!("Workspace not found: {id}"));
    }

    Ok(summarize_workspace(&workspace))
}

fn summarize_workspace(ws: &intent_core::Workspace) -> Value {
    json!({
        "id": ws.id.as_str(),
        "title": if ws.title.is_empty() { "Untitled" } else { &ws.title },
        "status": format!("{:?}", ws.status),
        "statusMessage": ws.status_message.as_deref(),
        "branch": &ws.branch,
        "baseRef": ws.base_ref.as_deref(),
        "repositoryPath": ws.repository_path.as_deref(),
        "repositoryOwner": ws.repository_owner.as_deref(),
        "repositoryName": ws.repository_name.as_deref(),
        "worktreePath": ws.worktree_path.as_deref(),
        "tags": &ws.tags,
        "createdAt": ws.created_at.as_str(),
        "updatedAt": ws.updated_at.as_str(),
        "lastActivity": ws.last_activity.as_deref(),
    })
}
