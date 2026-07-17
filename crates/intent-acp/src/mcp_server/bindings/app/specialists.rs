//! `ws.app.specialists.*` bindings (chief-gated).
//!
//! Exposes specialist read methods (`list`, `get`) exclusively to Chief-of-Staff
//! workspace agents. Non-chief agents receive a clear gating error. Uses the
//! existing 3-tier specialist loader in intent-services. Shape parity with the
//! TS reference
//! `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-specialists-api.ts`.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.specialists = {
        list: () => host({ method: 'app.specialists.list', args: {} }),
        get: (id) => host({ method: 'app.specialists.get', args: { id } }),
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
        other => Err(format!("host: unknown method `app.specialists.{other}`")),
    }
}

async fn list(api: &Arc<dyn WorkspaceApi>, _args: &Value) -> Result<Value, String> {
    // Fetch all specialists from the 3-tier loader (no workspace_path for chief)
    let result = api
        .specialist_list(None)
        .await
        .map_err(|e| format!("specialist.list failed: {e}"))?;

    // The daemon returns { specialists: SpecialistDef[] }
    let specialists = result
        .get("specialists")
        .and_then(Value::as_array)
        .ok_or_else(|| "specialists.list returned invalid shape".to_string())?;

    // The wire shape already matches the TS reference (id, name, description,
    // model, prompt, behaviorPrompt, source, isCustomized, etc.)
    Ok(Value::Array(specialists.clone()))
}

async fn get(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "Specialist id is required".to_string())?;

    // Fetch the specialist from the 3-tier loader
    let result = api
        .specialist_get(id.to_string(), None)
        .await
        .map_err(|e| {
            // Map NotFound to a clear error message
            if e.to_string().contains("not found") {
                format!("Specialist not found: {id}")
            } else {
                format!("specialist.get failed: {e}")
            }
        })?;

    // The daemon returns { specialist: SpecialistDef }
    let specialist = result
        .get("specialist")
        .ok_or_else(|| "specialists.get returned invalid shape".to_string())?;

    Ok(specialist.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Error, Result};
    use serde_json::json;
    use std::sync::Arc;

    #[derive(Default)]
    struct FakeApi {}

    impl WorkspaceApi for FakeApi {
        fn specialist_list(&self, _workspace_path: Option<String>) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Ok(json!({
                    "specialists": [
                        {
                            "id": "implementor",
                            "name": "Implementor",
                            "description": "Implements tasks",
                            "model": "claude-sonnet-4.5",
                            "prompt": "You are an implementor",
                            "behaviorPrompt": "Focus on implementation",
                            "source": "builtin",
                            "isCustomized": false
                        },
                        {
                            "id": "verifier",
                            "name": "Verifier",
                            "description": "Verifies work",
                            "model": "claude-sonnet-4.5",
                            "prompt": "You are a verifier",
                            "behaviorPrompt": "Focus on verification",
                            "source": "builtin",
                            "isCustomized": false
                        }
                    ]
                }))
            })
        }

        fn specialist_get(
            &self,
            id: String,
            _workspace_path: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                match id.as_str() {
                    "implementor" => Ok(json!({
                        "specialist": {
                            "id": "implementor",
                            "name": "Implementor",
                            "description": "Implements tasks",
                            "model": "claude-sonnet-4.5",
                            "prompt": "You are an implementor",
                            "behaviorPrompt": "Focus on implementation",
                            "source": "builtin",
                            "isCustomized": false
                        }
                    })),
                    _ => Err(Error::NotFound(format!("Specialist not found: {}", id))),
                }
            })
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
    async fn test_list_returns_expected_shape() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({})).await.unwrap();

        let specialists = result.as_array().unwrap();
        assert_eq!(specialists.len(), 2);

        // Check expected fields are present
        for specialist in specialists {
            assert!(specialist.get("id").is_some());
            assert!(specialist.get("name").is_some());
            assert!(specialist.get("description").is_some());
            assert!(specialist.get("model").is_some());
            assert!(specialist.get("prompt").is_some());
            assert!(specialist.get("behaviorPrompt").is_some());
            assert!(specialist.get("source").is_some());
            assert!(specialist.get("isCustomized").is_some());
        }
    }

    #[tokio::test]
    async fn test_get_returns_expected_shape() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "implementor" }))
            .await
            .unwrap();

        // Check expected fields are present
        assert_eq!(result.get("id").unwrap().as_str().unwrap(), "implementor");
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "Implementor");
        assert!(result.get("description").is_some());
        assert!(result.get("model").is_some());
        assert!(result.get("prompt").is_some());
        assert!(result.get("behaviorPrompt").is_some());
        assert!(result.get("source").is_some());
        assert!(result.get("isCustomized").is_some());
    }

    #[tokio::test]
    async fn test_get_missing_specialist_returns_error() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "id": "nonexistent" })).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Specialist not found: nonexistent");
    }
}
