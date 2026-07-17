//! `ws.app.specialists.*` bindings (chief-gated, placeholder).
//!
//! Placeholder module for future Wave 1 tasks. Returns a clear "not
//! implemented yet" error for all methods until the sibling task fills it.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.specialists = {};
"#;

pub(crate) async fn dispatch(
    _api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    method: &str,
    _args: &Value,
) -> Result<Value, String> {
    // Chief-workspace gating
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Chief of Staff workspace".to_string());
    }

    Err(format!(
        "ws.app.specialists.{method} is not implemented yet — a sibling Wave 1 task owns this namespace"
    ))
}
