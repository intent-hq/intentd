//! `ws.app.*` bindings (chief-gated).
//!
//! Namespace module for Chief-of-Staff workspace-only APIs. All `ws.app.*`
//! methods gate on `workspace_id.is_chief()` — non-chief agents receive a
//! clear "ws.app.* is only available in the Chief of Staff workspace" error.
//!
//! Structure: `app/mod.rs` owns the PRELUDE assembly and top-level dispatch
//! routing to submodules (`workspaces`, `agents`, `settings`, `specialists`).
//! Each submodule implements its own chief-gating check and dispatch.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

pub(crate) mod agents;
pub(crate) mod settings;
pub(crate) mod specialists;
pub(crate) mod workspaces;

/// Assemble the `ws.app.*` PRELUDE from all submodules. Each submodule
/// installs its portion of the namespace (e.g., `ws.app.workspaces`,
/// `ws.app.agents`). The prelude is unconditional; chief-gating is enforced
/// server-side in dispatch.
pub(crate) fn prelude() -> String {
    format!(
        "{}\n{}\n{}\n{}",
        workspaces::PRELUDE,
        agents::PRELUDE,
        settings::PRELUDE,
        specialists::PRELUDE,
    )
}

/// Dispatch one `ws.app.<subns>.<method>` call to the matching submodule.
/// Returns `Ok(None)` when the subnamespace is unknown; `Ok(Some(v))` on
/// success; `Err(msg)` on failure. The chief-workspace gate is delegated to
/// each submodule's dispatch so they can surface the same clear error.
pub(crate) async fn try_dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Option<Value>, String> {
    if let Some(rest) = method.strip_prefix("workspaces.") {
        return workspaces::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("agents.") {
        return agents::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("settings.") {
        return settings::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("specialists.") {
        return specialists::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    Ok(None)
}
