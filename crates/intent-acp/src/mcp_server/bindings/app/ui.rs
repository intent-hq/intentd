//! `ws.app.ui.*` bindings (chief-gated).
//!
//! Exposes app-UI surface for Chief-of-Staff workspace agents: navigate, highlight,
//! targets. Non-chief agents receive a clear gating error. Shape parity with the TS
//! reference `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-ui-api.ts`.

use std::sync::Arc;

use intent_core::{PublishEvent, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::mcp_server::bindings::map_err;

use crate::mcp_server::bindings::{opt_i64, opt_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.ui = {
        navigate: (route, options) => host({ method: 'app.ui.navigate', args: { route, ...(options || {}) } }),
        highlight: (id, options) => host({ method: 'app.ui.highlight', args: { id, ...(options || {}) } }),
        targets: () => host({ method: 'app.ui.targets', args: {} }),
    };
";

const MAX_HIGHLIGHT_DURATION_MS: i64 = 30_000;

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
        "navigate" => navigate(api, workspace_id, args).await,
        "highlight" => highlight(api, workspace_id, args).await,
        "targets" => Ok(targets()),
        other => Err(format!("host: unknown method `app.ui.{other}`")),
    }
}

async fn navigate(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let route = normalize_required_string(args, "route")?;
    let highlight_id_from_args = opt_str(args, "highlightId");
    let duration_ms = normalize_duration_ms(args, "durationMs")?;

    // Fallback: derive highlightId from route if not explicitly provided
    let highlight_id = highlight_id_from_args
        .or_else(|| get_highlight_id_from_route(&route))
        .filter(|s| !s.is_empty());

    // Build payload matching the FE AppUiNavigatePayload shape
    let mut payload = json!({
        "route": route,
        "workspaceId": workspace_id.as_str(),
    });
    if let Some(id) = &highlight_id {
        payload
            .as_object_mut()
            .unwrap()
            .insert("highlightId".to_string(), json!(id));
    }
    if let Some(duration) = duration_ms {
        payload
            .as_object_mut()
            .unwrap()
            .insert("durationMs".to_string(), json!(duration));
    }

    // Emit app:ui-navigate event
    let event = PublishEvent {
        workspace_id: workspace_id.clone(),
        event_type: intent_core::events::APP_UI_NAVIGATE.to_string(),
        data: payload.clone(),
    };
    api.publish_event(event).await.map_err(map_err)?;

    // Return { ok: true, ...payload }
    let mut result = payload;
    result
        .as_object_mut()
        .unwrap()
        .insert("ok".to_string(), json!(true));
    Ok(result)
}

async fn highlight(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let id = normalize_required_string(args, "id")?;
    let duration_ms = normalize_duration_ms(args, "durationMs")?;

    // Build payload matching the FE AppUiHighlightPayload shape
    let mut payload = json!({
        "id": id,
        "workspaceId": workspace_id.as_str(),
    });
    if let Some(duration) = duration_ms {
        payload
            .as_object_mut()
            .unwrap()
            .insert("durationMs".to_string(), json!(duration));
    }

    // Emit app:ui-highlight event
    let event = PublishEvent {
        workspace_id: workspace_id.clone(),
        event_type: intent_core::events::APP_UI_HIGHLIGHT.to_string(),
        data: payload.clone(),
    };
    api.publish_event(event).await.map_err(map_err)?;

    // Return { ok: true, ...payload }
    let mut result = payload;
    result
        .as_object_mut()
        .unwrap()
        .insert("ok".to_string(), json!(true));
    Ok(result)
}

fn targets() -> Value {
    // Port of APP_UI_TARGETS from packages/cloudlands-fe/src/shared/app-ui-targets.ts
    // DRIFT RISK: This is a static copy; changes to the FE targets table require
    // manual sync. Keep this in sync with the FE reference.
    json!([
        {
            "id": "home",
            "tab": "",
            "label": "Home",
            "route": "/",
            "category": "navigation",
            "description": "Workspace home and global overview."
        },
        {
            "id": "new-workspace",
            "tab": "",
            "label": "New workspace",
            "route": "/workspace/new",
            "category": "navigation",
            "description": "Create-workspace flow."
        },
        // Settings targets
        {
            "id": "quickActions.defaultModel",
            "tab": "agents",
            "hashAliases": ["default-model", "quickActions.defaultModel"],
            "scrollSelector": "#default-model",
            "highlightSelector": "[data-highlight-id=\"quickActions.defaultModel\"]",
            "label": "Settings: Default model",
            "route": "/settings?tab=agents#default-model",
            "category": "settings",
            "description": "Default AI behavior model selection."
        },
        {
            "id": "agents",
            "tab": "agents",
            "hashAliases": ["agents", "specialists", "all-agents"],
            "scrollSelector": "#specialists",
            "highlightSelector": "[data-highlight-id=\"specialists\"]",
            "label": "Settings: Agents",
            "route": "/settings?tab=agents#specialists",
            "category": "settings",
            "description": "Agent and specialist settings."
        },
        {
            "id": "workspace-card",
            "tab": "",
            "hashAliases": ["workspace-card"],
            "highlightSelector": "[data-highlight-id^=\"workspace-\"]",
            "label": "Workspace card",
            "route": "/",
            "category": "workspace",
            "description": "A workspace card on workspace list surfaces.",
            "dynamic": true,
            "idPattern": "workspace-{workspaceId}"
        },
    ])
}

/// Normalize a required string field (trim, non-empty check)
fn normalize_required_string(args: &Value, field: &str) -> Result<String, String> {
    let value = args
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{field} is required and must be a string"))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{field} cannot be empty"));
    }
    Ok(trimmed.to_string())
}

/// Normalize durationMs (must be 1..=30000)
fn normalize_duration_ms(args: &Value, field: &str) -> Result<Option<i64>, String> {
    let Some(duration) = opt_i64(args, field) else {
        return Ok(None);
    };
    if duration <= 0 || duration > MAX_HIGHLIGHT_DURATION_MS {
        return Err(format!(
            "{field} must be between 1 and {MAX_HIGHLIGHT_DURATION_MS}"
        ));
    }
    Ok(Some(duration))
}

/// Derive highlightId from route (simplified FE getHighlightIdFromRoute logic)
/// Extract hash from route, then try to resolve it via targets
fn get_highlight_id_from_route(route: &str) -> Option<String> {
    let hash = get_route_hash(route)?;
    let normalized = normalize_hash(&hash);
    if normalized.is_empty() {
        return None;
    }
    // Simple lookup: check if any target matches by id or hashAliases
    // For now, just return the normalized hash as the highlightId
    Some(normalized)
}

fn get_route_hash(route: &str) -> Option<String> {
    let hash_idx = route.find('#')?;
    Some(route[hash_idx + 1..].to_string())
}

fn normalize_hash(hash: &str) -> String {
    hash.trim_start_matches('#').trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        BoxFuture, Result, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
    };
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct FakeApi {
        events: Arc<Mutex<Vec<PublishEvent>>>,
    }

    impl FakeApi {
        fn published_events(&self) -> Vec<PublishEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl WorkspaceApi for FakeApi {
        fn get_workspace(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            Box::pin(async {
                Ok(Workspace {
                    id: WorkspaceId::from_string("ws-test"),
                    title: "Test".to_string(),
                    branch: "main".to_string(),
                    base_ref: None,
                    base_commit_sha: None,
                    status: WorkspaceStatus::Active,
                    status_message: None,
                    status_image_asset_id: None,
                    activity: WorkspaceActivity::Idle,
                    attention: WorkspaceAttention::None,
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    updated_at: "2026-01-01T00:00:00Z".to_string(),
                    last_activity: None,
                    tags: vec![],
                    path: None,
                    repository_path: None,
                    repository_owner: None,
                    repository_name: None,
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
                    display_status: None,
                    waiting: false,
                    checkout_mode: None,
                    disk_usage: None,
                    pending_delete_at: None,
                })
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

    #[tokio::test]
    async fn test_dispatch_rejects_non_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let result = dispatch(&api, &non_chief_id, "navigate", &json!({"route": "/"})).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_navigate_requires_route() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "navigate", &json!({})).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("route is required"));
    }

    #[tokio::test]
    async fn test_navigate_rejects_empty_route() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "navigate", &json!({"route": "  "})).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("route cannot be empty"));
    }

    #[tokio::test]
    async fn test_navigate_duration_ms_validates_range() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();

        // Below minimum
        let result = dispatch(
            &api,
            &chief_id,
            "navigate",
            &json!({"route": "/", "durationMs": 0}),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be between 1 and"));

        // Above maximum
        let result = dispatch(
            &api,
            &chief_id,
            "navigate",
            &json!({"route": "/", "durationMs": 40000}),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be between 1 and"));
    }

    #[tokio::test]
    async fn test_navigate_returns_expected_shape() {
        let fake = FakeApi::default();
        let api: Arc<dyn WorkspaceApi> = Arc::new(fake.clone());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "navigate", &json!({"route": "/settings"}))
            .await
            .unwrap();

        assert!(result.get("ok").unwrap().as_bool().unwrap());
        assert_eq!(result.get("route").unwrap().as_str().unwrap(), "/settings");
        assert_eq!(
            result.get("workspaceId").unwrap().as_str().unwrap(),
            "__chief__"
        );

        // Assert event was published
        let events = fake.published_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, intent_core::events::APP_UI_NAVIGATE);
        assert_eq!(events[0].workspace_id.as_str(), "__chief__");
        assert_eq!(
            events[0].data.get("route").unwrap().as_str().unwrap(),
            "/settings"
        );
    }

    #[tokio::test]
    async fn test_navigate_includes_highlight_id_from_option() {
        let fake = FakeApi::default();
        let api: Arc<dyn WorkspaceApi> = Arc::new(fake.clone());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "navigate",
            &json!({
                "route": "/settings",
                "highlightId": "agents"
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            result.get("highlightId").unwrap().as_str().unwrap(),
            "agents"
        );

        // Assert event includes highlightId
        let events = fake.published_events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].data.get("highlightId").unwrap().as_str().unwrap(),
            "agents"
        );
    }

    #[tokio::test]
    async fn test_highlight_requires_id() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "highlight", &json!({})).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("id is required"));
    }

    #[tokio::test]
    async fn test_highlight_returns_expected_shape() {
        let fake = FakeApi::default();
        let api: Arc<dyn WorkspaceApi> = Arc::new(fake.clone());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "highlight", &json!({"id": "agents"}))
            .await
            .unwrap();

        assert!(result.get("ok").unwrap().as_bool().unwrap());
        assert_eq!(result.get("id").unwrap().as_str().unwrap(), "agents");
        assert_eq!(
            result.get("workspaceId").unwrap().as_str().unwrap(),
            "__chief__"
        );

        // Assert event was published
        let events = fake.published_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, intent_core::events::APP_UI_HIGHLIGHT);
        assert_eq!(
            events[0].data.get("id").unwrap().as_str().unwrap(),
            "agents"
        );
    }

    #[tokio::test]
    async fn test_targets_returns_array() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "targets", &json!({}))
            .await
            .unwrap();

        assert!(result.is_array());
        let targets = result.as_array().unwrap();
        assert!(!targets.is_empty());

        // Check the first target has expected fields
        let first = &targets[0];
        assert!(first.get("id").is_some());
        assert!(first.get("tab").is_some());
        assert!(first.get("category").is_some());
    }
}
