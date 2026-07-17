//! `ws.app.settings.*` bindings (chief-gated).
//!
//! Exposes settings read methods (`list`, `get`) exclusively to Chief-of-Staff
//! workspace agents. Non-chief agents receive a clear gating error. Sensitive
//! settings are redacted exactly like the `settings.*` RPC surface (§5.12).
//! Shape parity with the TS reference
//! `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-settings-api.ts`.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::mcp_server::bindings::opt_bool;

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.settings = {
        list: (options) => host({ method: 'app.settings.list', args: options || {} }),
        get: (path) => host({ method: 'app.settings.get', args: { path } }),
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
        other => Err(format!("host: unknown method `app.settings.{other}`")),
    }
}

async fn list(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    // Extract options: includeValues (default true), category (optional filter)
    let include_values = opt_bool(args, "includeValues").unwrap_or(true);
    let category = args.get("category").and_then(Value::as_str);

    // Fetch all settings from the daemon (already redacted)
    let result = api
        .settings_list()
        .await
        .map_err(|e| format!("settings.list failed: {e}"))?;

    // Extract the settings array
    let all_settings = result
        .get("settings")
        .and_then(Value::as_array)
        .ok_or_else(|| "settings.list returned invalid shape".to_string())?;

    // Filter by category if specified
    let mut filtered = Vec::new();
    for setting in all_settings {
        if let Some(cat) = category {
            if setting.get("category").and_then(Value::as_str) != Some(cat) {
                continue;
            }
        }
        filtered.push(setting.clone());
    }

    // If includeValues is false, strip the value field from each setting
    if !include_values {
        let definitions: Vec<Value> = filtered
            .into_iter()
            .map(|mut s| {
                if let Some(obj) = s.as_object_mut() {
                    obj.remove("value");
                }
                s
            })
            .collect();
        return Ok(Value::Array(definitions));
    }

    Ok(Value::Array(filtered))
}

async fn get(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let path = args
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| "get(path) requires a setting path".to_string())?;

    // Fetch the setting (already redacted by the daemon)
    let result = api
        .settings_get(path.to_string())
        .await
        .map_err(|e| format!("settings.get failed: {e}"))?;

    // The daemon returns { path, value, definition }; we merge them into one
    // object matching the TS shape: definition fields + value + valueRedacted
    let definition = result
        .get("definition")
        .ok_or_else(|| "settings.get returned invalid shape".to_string())?;
    let value = result.get("value").cloned().unwrap_or(Value::Null);

    // Build the merged result
    let mut merged = definition.clone();
    if let Some(obj) = merged.as_object_mut() {
        obj.insert("value".into(), value);
        // Mark as redacted if the setting is sensitive
        let is_sensitive = obj.get("sensitive") == Some(&json!(true));
        obj.insert("valueRedacted".into(), json!(is_sensitive));
    }

    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Result};
    use serde_json::json;
    use std::sync::Arc;

    const REDACTED_PLACEHOLDER: &str = "***REDACTED***";

    #[derive(Default)]
    struct FakeApi {}

    impl WorkspaceApi for FakeApi {
        fn settings_list(&self) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Ok(json!({
                    "settings": [
                        {
                            "path": "user.name",
                            "value": "Alice",
                            "category": "user",
                            "sensitive": false,
                            "type": "string"
                        },
                        {
                            "path": "user.email",
                            "value": "alice@example.com",
                            "category": "user",
                            "sensitive": false,
                            "type": "string"
                        },
                        {
                            "path": "api.token",
                            "value": REDACTED_PLACEHOLDER,
                            "category": "api",
                            "sensitive": true,
                            "type": "string"
                        }
                    ]
                }))
            })
        }

        fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                match path.as_str() {
                    "user.name" => Ok(json!({
                        "path": "user.name",
                        "value": "Alice",
                        "definition": {
                            "path": "user.name",
                            "category": "user",
                            "sensitive": false,
                            "type": "string"
                        }
                    })),
                    "api.token" => Ok(json!({
                        "path": "api.token",
                        "value": REDACTED_PLACEHOLDER,
                        "definition": {
                            "path": "api.token",
                            "category": "api",
                            "sensitive": true,
                            "type": "string"
                        }
                    })),
                    _ => Err(intent_core::Error::NotFound(format!(
                        "Setting not found: {}",
                        path
                    ))),
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

        let settings = result.as_array().unwrap();
        assert_eq!(settings.len(), 3);

        // Check expected fields are present
        for setting in settings {
            assert!(setting.get("path").is_some());
            assert!(setting.get("value").is_some());
            assert!(setting.get("category").is_some());
            assert!(setting.get("sensitive").is_some());
            assert!(setting.get("type").is_some());
        }
    }

    #[tokio::test]
    async fn test_list_sensitive_values_redacted() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({})).await.unwrap();

        let settings = result.as_array().unwrap();
        let api_token = settings
            .iter()
            .find(|s| s.get("path").unwrap() == "api.token")
            .unwrap();

        // Sensitive setting should have redacted value
        assert_eq!(api_token.get("sensitive").unwrap().as_bool().unwrap(), true);
        assert_eq!(
            api_token.get("value").unwrap().as_str().unwrap(),
            REDACTED_PLACEHOLDER
        );
    }

    #[tokio::test]
    async fn test_list_filter_by_category() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({ "category": "user" }))
            .await
            .unwrap();

        let settings = result.as_array().unwrap();
        assert_eq!(settings.len(), 2); // Only user.name and user.email
        for setting in settings {
            assert_eq!(setting.get("category").unwrap().as_str().unwrap(), "user");
        }
    }

    #[tokio::test]
    async fn test_list_include_values_false() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "list", &json!({ "includeValues": false }))
            .await
            .unwrap();

        let settings = result.as_array().unwrap();
        assert_eq!(settings.len(), 3);

        // All settings should not have a value field
        for setting in settings {
            assert!(setting.get("value").is_none());
            assert!(setting.get("path").is_some());
            assert!(setting.get("category").is_some());
        }
    }

    #[tokio::test]
    async fn test_get_returns_expected_shape() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "path": "user.name" }))
            .await
            .unwrap();

        // Check expected fields are present
        assert_eq!(result.get("path").unwrap().as_str().unwrap(), "user.name");
        assert_eq!(result.get("value").unwrap().as_str().unwrap(), "Alice");
        assert_eq!(
            result.get("valueRedacted").unwrap().as_bool().unwrap(),
            false
        );
        assert!(result.get("category").is_some());
        assert!(result.get("sensitive").is_some());
        assert!(result.get("type").is_some());
    }

    #[tokio::test]
    async fn test_get_sensitive_value_redacted() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "path": "api.token" }))
            .await
            .unwrap();

        // Sensitive setting should have redacted value and valueRedacted=true
        assert_eq!(result.get("sensitive").unwrap().as_bool().unwrap(), true);
        assert_eq!(
            result.get("value").unwrap().as_str().unwrap(),
            REDACTED_PLACEHOLDER
        );
        assert_eq!(
            result.get("valueRedacted").unwrap().as_bool().unwrap(),
            true
        );
    }

    #[tokio::test]
    async fn test_get_non_sensitive_value_not_redacted() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "path": "user.name" }))
            .await
            .unwrap();

        // Non-sensitive setting should have real value and valueRedacted=false
        assert_eq!(result.get("sensitive").unwrap().as_bool().unwrap(), false);
        assert_eq!(result.get("value").unwrap().as_str().unwrap(), "Alice");
        assert_eq!(
            result.get("valueRedacted").unwrap().as_bool().unwrap(),
            false
        );
    }
}
