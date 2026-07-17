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
