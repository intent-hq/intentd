//! `ws.app.workspaces.*` bindings (chief-gated).
//!
//! Exposes workspace management methods (`list`, `get`) exclusively to
//! Chief-of-Staff workspace agents. Non-chief agents receive a clear gating
//! error. Shape parity with the TS reference
//! `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-workspaces-api.ts`.

use std::sync::Arc;

use intent_core::{PublishEvent, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::mcp_server::bindings::{map_err, opt_bool, opt_str, opt_vec_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.workspaces = {
        list: (options) => host({ method: 'app.workspaces.list', args: options || {} }),
        get: (id) => host({ method: 'app.workspaces.get', args: { id } }),
        create: (params) => host({ method: 'app.workspaces.create', args: params || {} }),
        archive: (id) => host({ method: 'app.workspaces.archive', args: { id } }),
        delete: (id) => host({ method: 'app.workspaces.delete', args: { id } }),
        open: (id, options) => host({ method: 'app.workspaces.open', args: { id, ...(options || {}) } }),
        bulkArchive: (ids) => host({ method: 'app.workspaces.bulkArchive', args: { ids } }),
        bulkDelete: (ids) => host({ method: 'app.workspaces.bulkDelete', args: { ids } }),
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
        "create" => create(api, args).await,
        "archive" => archive(api, args).await,
        "delete" => delete(api, args).await,
        "open" => open(api, workspace_id, args).await,
        "bulkArchive" => bulk_archive(api, args).await,
        "bulkDelete" => bulk_delete(api, args).await,
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

async fn open(
    api: &Arc<dyn WorkspaceApi>,
    _caller_workspace_id: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    assert_mutable_workspace_id(id)?;

    let open_in_new_window = opt_bool(args, "openInNewWindow").unwrap_or(false);

    // Validate workspace exists
    let workspace_id = WorkspaceId::from_string(id.to_string());
    let _workspace = api
        .get_workspace(workspace_id.clone())
        .await
        .map_err(map_err)?;

    // Emit app:workspace-open event
    let event_data = json!({
        "workspaceId": id,
        "openInNewWindow": open_in_new_window,
    });
    let event = PublishEvent {
        workspace_id: workspace_id.clone(),
        event_type: intent_core::events::APP_WORKSPACE_OPEN.to_string(),
        data: event_data,
    };
    api.publish_event(event).await.map_err(map_err)?;

    Ok(json!({
        "ok": true,
        "queued": true,
    }))
}

/// MCP resource MIME type for proposals (parity with FE `proposal-resource.ts`).
const PROPOSAL_RESOURCE_MIME_TYPE: &str = "application/vnd.intent.proposal+json";

/// Build proposal resource URI (parity with TS `proposalResourceId` + `createProposalResource`).
fn proposal_resource_uri(proposal: &Value) -> String {
    let kind = proposal
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    // Use applyToolCallId if present, otherwise use preview.title
    let id = proposal
        .get("applyToolCallId")
        .and_then(Value::as_str)
        .or_else(|| {
            proposal
                .get("preview")
                .and_then(|p| p.get("title"))
                .and_then(Value::as_str)
        })
        .unwrap_or("untitled");

    // URL-encode the id portion (simplified - production would use proper URL encoding)
    let encoded_id = id.replace('/', "%2F").replace(' ', "%20");
    format!("intent-proposal://{}/{}", kind, encoded_id)
}

/// Return a proposal with dual text+resource content items.
fn proposal_result(proposal: Value) -> Result<Value, String> {
    // Build resource name from preview.title
    let name = proposal
        .get("preview")
        .and_then(|p| p.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("Proposal");

    // Build MCP content items: text item with {ok, proposal} + resource item
    let text_item = json!({
        "type": "text",
        "text": serde_json::to_string_pretty(&json!({
            "ok": true,
            "proposal": proposal
        })).unwrap_or_else(|_| "{}".to_string())
    });

    let resource_item = json!({
        "type": "resource",
        "resource": {
            "uri": proposal_resource_uri(&proposal),
            "name": name,
            "mimeType": PROPOSAL_RESOURCE_MIME_TYPE,
            "text": serde_json::to_string(&proposal).unwrap_or_else(|_| "{}".to_string())
        }
    });

    // Return with __mcpContentItems marker (dispatch.rs will extract this)
    Ok(json!({
        "ok": true,
        "proposal": proposal,
        "__mcpContentItems": [text_item, resource_item]
    }))
}

/// Validate that a workspace ID is mutable (not __chief__).
fn assert_mutable_workspace_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("workspace id is required".to_string());
    }
    if id == "__chief__" {
        return Err("The Chief virtual workspace cannot be modified".to_string());
    }
    Ok(())
}

async fn create(_api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    // Build workspace-create proposal from params
    let params = args.as_object().cloned().unwrap_or_default();

    // Extract title for preview
    let title = params
        .get("title")
        .and_then(Value::as_str)
        .map(|s| format!(": {}", s))
        .unwrap_or_default();

    let proposal = json!({
        "kind": "workspace-create",
        "payload": {
            "operation": "workspace.create",
            "params": params
        },
        "preview": {
            "title": format!("Create workspace{}", title),
            "summary": "Review and adjust workspace creation details before creating a new space.",
            "workspaceCreate": {}
        }
    });

    proposal_result(proposal)
}

async fn archive(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    assert_mutable_workspace_id(id)?;

    // Validate workspace exists
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let workspace = workspaces
        .iter()
        .find(|w| w.id.as_str() == id)
        .ok_or_else(|| format!("Workspace not found: {id}"))?;

    // Build bulk-op proposal with single item
    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkArchive",
            "ids": [id]
        },
        "preview": {
            "title": "Archive 1 workspace",
            "summary": "Review the selected workspaces before archiving them.",
            "applyLabel": "Archive",
            "bulkItems": [{
                "id": id,
                "title": if workspace.title.is_empty() { id } else { &workspace.title },
                "summary": workspace.repository_name.as_deref()
                    .or(workspace.repository_path.as_deref())
                    .or(Some(format!("{:?}", workspace.status).as_str())),
                "selected": true,
                "metadata": summarize_workspace(workspace)
            }]
        }
    });

    proposal_result(proposal)
}

async fn delete(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "id is required".to_string())?;

    assert_mutable_workspace_id(id)?;

    // Validate workspace exists
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let workspace = workspaces
        .iter()
        .find(|w| w.id.as_str() == id)
        .ok_or_else(|| format!("Workspace not found: {id}"))?;

    // Build bulk-op proposal with single item
    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkDelete",
            "ids": [id]
        },
        "preview": {
            "title": "Delete 1 workspace",
            "summary": "Review the selected workspaces before deleting them.",
            "applyLabel": "Delete",
            "bulkItems": [{
                "id": id,
                "title": if workspace.title.is_empty() { id } else { &workspace.title },
                "summary": workspace.repository_name.as_deref()
                    .or(workspace.repository_path.as_deref())
                    .or(Some(format!("{:?}", workspace.status).as_str())),
                "selected": true,
                "metadata": summarize_workspace(workspace)
            }],
            "warnings": ["Deleting workspaces is destructive. Confirm the selected workspaces before applying."]
        }
    });

    proposal_result(proposal)
}

async fn bulk_archive(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let ids = args
        .get("ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "ids must be a non-empty array".to_string())?;

    if ids.is_empty() {
        return Err("ids must be a non-empty array".to_string());
    }

    // Validate all IDs are mutable
    for id_val in ids {
        if let Some(id) = id_val.as_str() {
            assert_mutable_workspace_id(id)?;
        } else {
            return Err("all ids must be strings".to_string());
        }
    }

    // Validate all workspaces exist
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let mut bulk_items = Vec::new();

    for id_val in ids {
        let id = id_val.as_str().unwrap(); // Already validated above
        let workspace = workspaces
            .iter()
            .find(|w| w.id.as_str() == id)
            .ok_or_else(|| format!("Workspace not found: {id}"))?;

        bulk_items.push(json!({
            "id": id,
            "title": if workspace.title.is_empty() { id } else { &workspace.title },
            "summary": workspace.repository_name.as_deref()
                .or(workspace.repository_path.as_deref())
                .or(Some(format!("{:?}", workspace.status).as_str())),
            "selected": true,
            "metadata": summarize_workspace(workspace)
        }));
    }

    let count = ids.len();
    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkArchive",
            "ids": ids
        },
        "preview": {
            "title": format!("Archive {} workspace{}", count, if count == 1 { "" } else { "s" }),
            "summary": "Review the selected workspaces before archiving them.",
            "applyLabel": "Archive",
            "bulkItems": bulk_items
        }
    });

    proposal_result(proposal)
}

async fn bulk_delete(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let ids = args
        .get("ids")
        .and_then(Value::as_array)
        .ok_or_else(|| "ids must be a non-empty array".to_string())?;

    if ids.is_empty() {
        return Err("ids must be a non-empty array".to_string());
    }

    // Validate all IDs are mutable
    for id_val in ids {
        if let Some(id) = id_val.as_str() {
            assert_mutable_workspace_id(id)?;
        } else {
            return Err("all ids must be strings".to_string());
        }
    }

    // Validate all workspaces exist
    let workspaces = api.list_workspaces(true).await.map_err(map_err)?;
    let mut bulk_items = Vec::new();

    for id_val in ids {
        let id = id_val.as_str().unwrap(); // Already validated above
        let workspace = workspaces
            .iter()
            .find(|w| w.id.as_str() == id)
            .ok_or_else(|| format!("Workspace not found: {id}"))?;

        bulk_items.push(json!({
            "id": id,
            "title": if workspace.title.is_empty() { id } else { &workspace.title },
            "summary": workspace.repository_name.as_deref()
                .or(workspace.repository_path.as_deref())
                .or(Some(format!("{:?}", workspace.status).as_str())),
            "selected": true,
            "metadata": summarize_workspace(workspace)
        }));
    }

    let count = ids.len();
    let proposal = json!({
        "kind": "bulk-op",
        "payload": {
            "operation": "workspace.bulkDelete",
            "ids": ids
        },
        "preview": {
            "title": format!("Delete {} workspace{}", count, if count == 1 { "" } else { "s" }),
            "summary": "Review the selected workspaces before deleting them.",
            "applyLabel": "Delete",
            "bulkItems": bulk_items,
            "warnings": ["Deleting workspaces is destructive. Confirm the selected workspaces before applying."]
        }
    });

    proposal_result(proposal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        BoxFuture, Error, Result, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
    };
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeApi {
        workspaces: Arc<Mutex<Vec<Workspace>>>,
        events: Arc<Mutex<Vec<PublishEvent>>>,
    }

    impl FakeApi {
        fn published_events(&self) -> Vec<PublishEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl WorkspaceApi for FakeApi {
        fn list_workspaces(
            &self,
            _include_archived: bool,
        ) -> BoxFuture<'_, Result<Vec<Workspace>>> {
            let workspaces = self.workspaces.lock().unwrap().clone();
            Box::pin(async move { Ok(workspaces) })
        }

        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let workspaces = self.workspaces.lock().unwrap().clone();
            Box::pin(async move {
                workspaces
                    .into_iter()
                    .find(|w| w.id == id)
                    .ok_or_else(|| Error::NotFound(format!("Workspace not found: {}", id.as_str())))
            })
        }

        fn publish_event(&self, event: PublishEvent) -> BoxFuture<'_, Result<()>> {
            let events = self.events.clone();
            Box::pin(async move {
                events.lock().unwrap().push(event);
                Ok(())
            })
        }
    }

    fn make_workspace(id: &str, title: &str) -> Workspace {
        Workspace {
            id: WorkspaceId::from_string(id),
            title: title.to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: Some("/repo".to_string()),
            repository_owner: Some("owner".to_string()),
            repository_name: Some("repo".to_string()),
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
        }
    }

    #[tokio::test]
    async fn test_dispatch_rejects_non_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let result = dispatch(&api, &non_chief_id, "list", &json!({})).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_list_excludes_chief_workspace() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("__chief__", "Chief of Staff"));
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            workspaces.push(make_workspace("ws-2", "Workspace 2"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({})).await.unwrap();
        let workspaces = result.as_array().unwrap();

        // __chief__ should not appear in results
        assert_eq!(workspaces.len(), 2);
        assert!(workspaces
            .iter()
            .all(|w| w.get("id").unwrap().as_str().unwrap() != "__chief__"));
    }

    #[tokio::test]
    async fn test_get_missing_workspace_returns_error() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Workspace not found: missing-ws"));
    }

    #[tokio::test]
    async fn test_get_chief_workspace_returns_error() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("__chief__", "Chief of Staff"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "__chief__" })).await;
        // Even if chief exists in the list, get should reject it
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Workspace not found: __chief__");
    }

    #[tokio::test]
    async fn test_list_returns_expected_shape() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({})).await.unwrap();
        let workspaces = result.as_array().unwrap();
        assert_eq!(workspaces.len(), 1);

        let ws = &workspaces[0];
        // Check expected fields are present
        assert!(ws.get("id").is_some());
        assert!(ws.get("title").is_some());
        assert!(ws.get("status").is_some());
        assert!(ws.get("branch").is_some());
        assert!(ws.get("tags").is_some());
        assert!(ws.get("createdAt").is_some());
        assert!(ws.get("updatedAt").is_some());
    }

    #[tokio::test]
    async fn test_list_filter_by_status() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            let mut ws_active = make_workspace("ws-1", "Active");
            ws_active.status = WorkspaceStatus::Active;
            workspaces.push(ws_active);

            let mut ws_archived = make_workspace("ws-2", "Archived");
            ws_archived.status = WorkspaceStatus::Archived;
            workspaces.push(ws_archived);
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "list",
            &json!({ "filter": { "status": ["active"] } }),
        )
        .await
        .unwrap();
        let workspaces = result.as_array().unwrap();
        assert_eq!(workspaces.len(), 1);
        assert_eq!(workspaces[0].get("id").unwrap().as_str().unwrap(), "ws-1");
    }

    #[tokio::test]
    async fn test_list_sort_by_title() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Zebra"));
            workspaces.push(make_workspace("ws-2", "Apple"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "list",
            &json!({ "sort": { "by": "title", "order": "asc" } }),
        )
        .await
        .unwrap();
        let workspaces = result.as_array().unwrap();
        assert_eq!(workspaces.len(), 2);
        assert_eq!(
            workspaces[0].get("title").unwrap().as_str().unwrap(),
            "Apple"
        );
        assert_eq!(
            workspaces[1].get("title").unwrap().as_str().unwrap(),
            "Zebra"
        );
    }

    #[tokio::test]
    async fn test_get_returns_expected_shape() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        // Check expected fields are present
        assert_eq!(result.get("id").unwrap().as_str().unwrap(), "ws-1");
        assert_eq!(
            result.get("title").unwrap().as_str().unwrap(),
            "Test Workspace"
        );
        assert!(result.get("status").is_some());
        assert!(result.get("branch").is_some());
        assert!(result.get("tags").is_some());
        assert!(result.get("createdAt").is_some());
        assert!(result.get("updatedAt").is_some());
    }

    // Proposal methods tests

    #[tokio::test]
    async fn test_create_returns_proposal() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "create", &json!({ "title": "New WS" }))
            .await
            .unwrap();

        // Should have proposal and content items
        assert_eq!(result.get("ok").unwrap().as_bool().unwrap(), true);
        let proposal = result.get("proposal").unwrap();
        assert_eq!(
            proposal.get("kind").unwrap().as_str().unwrap(),
            "workspace-create"
        );
        assert!(proposal.get("preview").is_some());
        assert!(proposal.get("payload").is_some());

        let items = result.get("__mcpContentItems").unwrap().as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].get("type").unwrap().as_str().unwrap(), "text");
        assert_eq!(items[1].get("type").unwrap().as_str().unwrap(), "resource");
    }

    #[tokio::test]
    async fn test_archive_rejects_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "__chief__" })).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_archive_validates_workspace_exists() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Workspace not found: missing-ws"));
    }

    #[tokio::test]
    async fn test_archive_returns_proposal() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        // Should have proposal and content items
        assert_eq!(result.get("ok").unwrap().as_bool().unwrap(), true);
        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkArchive"
        );
        assert_eq!(payload.get("ids").unwrap().as_array().unwrap().len(), 1);

        let preview = proposal.get("preview").unwrap();
        assert_eq!(
            preview.get("title").unwrap().as_str().unwrap(),
            "Archive 1 workspace"
        );
    }

    #[tokio::test]
    async fn test_delete_rejects_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "delete", &json!({ "id": "__chief__" })).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_delete_validates_workspace_exists() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "delete", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Workspace not found: missing-ws"));
    }

    #[tokio::test]
    async fn test_delete_returns_proposal_with_warnings() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "delete", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkDelete"
        );

        let preview = proposal.get("preview").unwrap();
        assert_eq!(
            preview.get("title").unwrap().as_str().unwrap(),
            "Delete 1 workspace"
        );
        assert!(preview.get("warnings").is_some());
    }

    #[tokio::test]
    async fn test_bulk_archive_rejects_empty_array() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "bulkArchive", &json!({ "ids": [] })).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ids must be a non-empty array");
    }

    #[tokio::test]
    async fn test_bulk_archive_rejects_chief_in_list() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "bulkArchive",
            &json!({ "ids": ["ws-1", "__chief__"] }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_bulk_archive_validates_all_exist() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "WS 1"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "bulkArchive",
            &json!({ "ids": ["ws-1", "ws-2"] }),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Workspace not found: ws-2"));
    }

    #[tokio::test]
    async fn test_bulk_archive_returns_proposal() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "WS 1"));
            workspaces.push(make_workspace("ws-2", "WS 2"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "bulkArchive",
            &json!({ "ids": ["ws-1", "ws-2"] }),
        )
        .await
        .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkArchive"
        );
        assert_eq!(payload.get("ids").unwrap().as_array().unwrap().len(), 2);

        let preview = proposal.get("preview").unwrap();
        assert_eq!(
            preview.get("title").unwrap().as_str().unwrap(),
            "Archive 2 workspaces"
        );

        let bulk_items = preview.get("bulkItems").unwrap().as_array().unwrap();
        assert_eq!(bulk_items.len(), 2);
    }

    #[tokio::test]
    async fn test_bulk_delete_rejects_empty_array() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "bulkDelete", &json!({ "ids": [] })).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ids must be a non-empty array");
    }

    #[tokio::test]
    async fn test_bulk_delete_returns_proposal_with_warnings() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "WS 1"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "bulkDelete", &json!({ "ids": ["ws-1"] }))
            .await
            .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(proposal.get("kind").unwrap().as_str().unwrap(), "bulk-op");

        let payload = proposal.get("payload").unwrap();
        assert_eq!(
            payload.get("operation").unwrap().as_str().unwrap(),
            "workspace.bulkDelete"
        );

        let preview = proposal.get("preview").unwrap();
        assert!(preview.get("warnings").is_some());
    }

    #[tokio::test]
    async fn test_proposal_has_mcp_content_items() {
        let fake = Arc::new(FakeApi::default());
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test"));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "archive", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        let items = result.get("__mcpContentItems").unwrap().as_array().unwrap();
        assert_eq!(items.len(), 2);

        // Text item
        assert_eq!(items[0].get("type").unwrap().as_str().unwrap(), "text");
        let text = items[0].get("text").unwrap().as_str().unwrap();
        assert!(text.contains("\"ok\": true"));

        // Resource item
        assert_eq!(items[1].get("type").unwrap().as_str().unwrap(), "resource");
        let resource = items[1].get("resource").unwrap();
        assert_eq!(
            resource.get("mimeType").unwrap().as_str().unwrap(),
            "application/vnd.intent.proposal+json"
        );
        assert!(resource
            .get("uri")
            .unwrap()
            .as_str()
            .unwrap()
            .starts_with("intent-proposal://"));
    }

    #[tokio::test]
    async fn test_open_rejects_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "open", &json!({ "id": "__chief__" })).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "The Chief virtual workspace cannot be modified"
        );
    }

    #[tokio::test]
    async fn test_open_validates_workspace_exists() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "open", &json!({ "id": "missing-ws" })).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Workspace not found"));
    }

    #[tokio::test]
    async fn test_open_returns_expected_shape() {
        let fake = FakeApi::default();
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test Workspace"));
        }
        let api: Arc<dyn WorkspaceApi> = Arc::new(fake.clone());

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "open", &json!({ "id": "ws-1" }))
            .await
            .unwrap();

        assert_eq!(result.get("ok").unwrap().as_bool().unwrap(), true);
        assert_eq!(result.get("queued").unwrap().as_bool().unwrap(), true);

        // Assert event was published
        let events = fake.published_events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].event_type,
            intent_core::events::APP_WORKSPACE_OPEN
        );
        assert_eq!(
            events[0].data.get("workspaceId").unwrap().as_str().unwrap(),
            "ws-1"
        );
        assert_eq!(
            events[0]
                .data
                .get("openInNewWindow")
                .unwrap()
                .as_bool()
                .unwrap(),
            false
        );
    }

    #[tokio::test]
    async fn test_open_accepts_open_in_new_window_option() {
        let fake = FakeApi::default();
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Test"));
        }
        let api: Arc<dyn WorkspaceApi> = Arc::new(fake.clone());

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "open",
            &json!({ "id": "ws-1", "openInNewWindow": true }),
        )
        .await
        .unwrap();

        assert_eq!(result.get("ok").unwrap().as_bool().unwrap(), true);
        assert_eq!(result.get("queued").unwrap().as_bool().unwrap(), true);

        // Assert event includes openInNewWindow
        let events = fake.published_events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]
                .data
                .get("openInNewWindow")
                .unwrap()
                .as_bool()
                .unwrap(),
            true
        );
    }
}
