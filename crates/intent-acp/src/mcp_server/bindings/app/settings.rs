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
