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

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.settings = {
        list: (options) => host({ method: 'app.settings.list', args: options || {} }),
        get: (path) => host({ method: 'app.settings.get', args: { path } }),
        propose: (input) => host({ method: 'app.settings.propose', args: input }),
    };
";

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
        "propose" => propose(api, args).await,
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

    // RFC3986 percent-encode the id portion for URI path segment use
    let encoded_id = super::proposal::percent_encode_path_segment(id);
    format!("intent-proposal://{kind}/{encoded_id}")
}

/// Return a proposal with dual text+resource content items.
#[allow(clippy::unnecessary_wraps)] // dispatch arm helper; keeps the uniform Result shape
fn proposal_result(proposal: &Value) -> Result<Value, String> {
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
            "uri": proposal_resource_uri(proposal),
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

async fn propose(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    // Extract changes from input (can be array or { changes: [...] })
    let changes = if let Some(arr) = args.as_array() {
        arr.clone()
    } else if let Some(changes_arr) = args.get("changes").and_then(Value::as_array) {
        changes_arr.clone()
    } else {
        return Err("propose() requires an array of changes or { changes: [...] }".to_string());
    };

    if changes.is_empty() {
        return Err("propose() requires at least one change".to_string());
    }

    // Validate each change and gather current values
    let mut validated_changes = Vec::new();

    for change in &changes {
        let path = change
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "Each change must have a 'path' field".to_string())?;

        if change.get("value").is_none() {
            return Err(format!("Change for '{path}' must have a 'value' field"));
        }

        // Get setting definition to validate
        let setting_result = api
            .settings_get(path.to_string())
            .await
            .map_err(|e| match e {
                intent_core::Error::NotFound(_) => format!("Unknown app setting path: {path}"),
                _ => format!("settings.get failed: {e}"),
            })?;

        let definition = setting_result
            .get("definition")
            .ok_or_else(|| "settings.get returned invalid shape".to_string())?;

        // Check if setting is sensitive
        if definition.get("sensitive") == Some(&json!(true)) {
            return Err(format!(
                "Invalid app setting change: {path} setting is sensitive and cannot be changed via MCP proposals"
            ));
        }

        validated_changes.push((
            change,
            definition.clone(),
            setting_result.get("value").cloned(),
        ));
    }

    // Build proposal
    let payload_changes: Vec<Value> = validated_changes
        .iter()
        .map(|(change, definition, _)| {
            json!({
                "path": change.get("path"),
                "value": change.get("value"),
                "reason": change.get("reason"),
                "apply": definition.get("apply")
            })
        })
        .collect();

    // Format preview fields
    let fields: Vec<Value> = validated_changes
        .iter()
        .map(|(change, definition, current_value)| {
            let label = definition
                .get("label")
                .and_then(Value::as_str)
                .unwrap_or("Setting");
            let new_value = change.get("value").unwrap();
            let value_str = new_value.as_str().map_or_else(
                || serde_json::to_string(new_value).unwrap(),
                std::string::ToString::to_string,
            );
            let before_str = current_value
                .as_ref()
                .and_then(|v| v.as_str().map(std::string::ToString::to_string))
                .or_else(|| {
                    current_value
                        .as_ref()
                        .map(|v| serde_json::to_string(v).unwrap())
                })
                .unwrap_or_else(|| "null".to_string());

            let is_multiline = matches!(
                definition.get("type").and_then(Value::as_str),
                Some("object" | "array")
            );

            json!({
                "key": change.get("path"),
                "label": label,
                "before": before_str,
                "after": value_str,
                "editable": true,
                "multiline": is_multiline
            })
        })
        .collect();

    // Build title and summary
    let (title, summary) = if validated_changes.len() == 1 {
        let (_, definition, _) = &validated_changes[0];
        let label = definition
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or("Setting");
        let value_str = format!("{}", validated_changes[0].0.get("value").unwrap());
        (
            format!("{label}: {value_str}"),
            format!("Switch the {} to {}.", label.to_lowercase(), value_str),
        )
    } else {
        let labels: Vec<&str> = validated_changes
            .iter()
            .filter_map(|(_, def, _)| def.get("label").and_then(Value::as_str))
            .collect();
        (
            format!("Update {} settings", validated_changes.len()),
            labels.join(", "),
        )
    };

    let proposal = json!({
        "kind": "settings-change",
        "payload": {
            "changes": payload_changes
        },
        "preview": {
            "title": title,
            "summary": summary,
            "applyLabel": "Apply",
            "fields": fields
        }
    });

    proposal_result(&proposal)
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
                        "Setting not found: {path}"
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
        assert!(api_token.get("sensitive").unwrap().as_bool().unwrap());
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
        assert!(!result.get("valueRedacted").unwrap().as_bool().unwrap());
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
        assert!(result.get("sensitive").unwrap().as_bool().unwrap());
        assert_eq!(
            result.get("value").unwrap().as_str().unwrap(),
            REDACTED_PLACEHOLDER
        );
        assert!(result.get("valueRedacted").unwrap().as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_get_non_sensitive_value_not_redacted() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "get", &json!({ "path": "user.name" }))
            .await
            .unwrap();

        // Non-sensitive setting should have real value and valueRedacted=false
        assert!(!result.get("sensitive").unwrap().as_bool().unwrap());
        assert_eq!(result.get("value").unwrap().as_str().unwrap(), "Alice");
        assert!(!result.get("valueRedacted").unwrap().as_bool().unwrap());
    }

    // Proposal tests

    #[tokio::test]
    async fn test_propose_rejects_non_chief() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let result = dispatch(
            &api,
            &non_chief_id,
            "propose",
            &json!([{ "path": "user.name", "value": "Bob" }]),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_propose_requires_changes() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "propose", &json!({})).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("propose() requires an array of changes"));
    }

    #[tokio::test]
    async fn test_propose_rejects_empty_changes() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, "propose", &json!([])).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("propose() requires at least one change"));
    }

    #[tokio::test]
    async fn test_propose_rejects_unknown_path() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!([{ "path": "unknown.setting", "value": "test" }]),
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Unknown app setting path: unknown.setting"));
    }

    #[tokio::test]
    async fn test_propose_rejects_sensitive_setting() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!([{ "path": "api.token", "value": "new-token" }]),
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("setting is sensitive and cannot be changed via MCP proposals"));
    }

    #[tokio::test]
    async fn test_propose_returns_proposal() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!([{ "path": "user.name", "value": "Bob" }]),
        )
        .await
        .unwrap();

        // Should have proposal and content items
        assert!(result.get("ok").unwrap().as_bool().unwrap());
        let proposal = result.get("proposal").unwrap();
        assert_eq!(
            proposal.get("kind").unwrap().as_str().unwrap(),
            "settings-change"
        );

        let payload = proposal.get("payload").unwrap();
        let changes = payload.get("changes").unwrap().as_array().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].get("path").unwrap().as_str().unwrap(),
            "user.name"
        );

        let preview = proposal.get("preview").unwrap();
        assert!(preview.get("title").is_some());
        assert!(preview.get("summary").is_some());
        assert!(preview.get("fields").is_some());

        let items = result.get("__mcpContentItems").unwrap().as_array().unwrap();
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn test_propose_accepts_object_with_changes() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!({ "changes": [{ "path": "user.name", "value": "Bob" }] }),
        )
        .await
        .unwrap();

        let proposal = result.get("proposal").unwrap();
        assert_eq!(
            proposal.get("kind").unwrap().as_str().unwrap(),
            "settings-change"
        );
    }

    #[tokio::test]
    async fn test_propose_has_mcp_content_items() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            "propose",
            &json!([{ "path": "user.name", "value": "Bob" }]),
        )
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
}
